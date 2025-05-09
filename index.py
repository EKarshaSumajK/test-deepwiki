from datetime import datetime
from bson import ObjectId
from fastapi import FastAPI, APIRouter, HTTPException, Request, status
from fastapi.params import Depends, Path, Query

from app.auth.authorization import verify_user,check_user_authorization
from app.connectors.mongo import MongoDBConnector
from app.connectors.storage import CloudStorage
from app.models.models import Assessment, EditAssessment, ListAssessments, AssessmentId, AssessmentMarks, AssessmentCourse,AssessmentSubmission, CourseList,AssessmentSubmissionCheck,SchoolIDRequest
from app.logger import logger

from app.utils.course import Course
from app.utils.latex_render import LatexRenderer
from app.utils.mongo_query import QueryGenerator
from app.models.question_models import AddQuestionsRequest
from app.config.config import config

class Assessments:

    def __init__(self):
        self.router = APIRouter()
        self.router.add_api_route("/{assesment_type}/create", self.create_assessment, methods=["POST"])
        self.router.add_api_route("/{assesment_type}/list", self.list_assessments, methods=["POST"])
        self.router.add_api_route("/{assesment_type}/details", self.get_assessment_details, methods=["POST"])
        self.router.add_api_route("/{assesment_type}/generate", self.generate_assessment, methods=["POST"])
        self.router.add_api_route("/{assesment_type}/edit", self.edit_assessment, methods=["POST"])
        self.router.add_api_route("/{assesment_type}/delete", self.delete_assessment, methods=["DELETE"])
        

        self.latex_renderer = LatexRenderer()
        self.mongo_driver = MongoDBConnector()
        self.query_generator = QueryGenerator()

    async def create_assessment(self, request:Request, assessment: Assessment, assesment_type: str = Path(..., title="The type of assessment"), current_user: dict = Depends(check_user_authorization)):
        """
        This takes a list of assessment questions and creates the assessment pdf
        """
        try:
            await logger.log_message(request=request, message=f"Authenticated user")

            # Validate required fields
            if not assessment.questions or len(assessment.questions) == 0:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Assessment must contain at least one question"
                )

            # Unpacking QueryParams
            assessment_data = {
                "name": assessment.name,
                "course": assessment.course,
                "start_date": assessment.start_date,
                "end_date": assessment.end_date,
                "total_time": assessment.total_time,     
                "total_marks": assessment.total_marks,
                "lessons": assessment.lessons,
                "questions": [
                    {
                        "section_name": section.section_name,
                        "description": section.description,
                        "questions": [
                            {
                                "question_id": question.question_id,
                                "question_latex": question.question_latex,
                                "marks": question.marks
                            }
                            for question in section.questions
                        ]
                    }
                    for section in assessment.questions
                ],
                "last_updated": datetime.utcnow()
            }

            # Generate a new ObjectId for the assessment
            assessment_id = str(ObjectId())
            assessment_data['_id'] = ObjectId(assessment_id)  # Set the generated ID in the document
            await logger.log_message(request=request, message=f"Generated assessment ID: {assessment_id}")

            await logger.log_message(request=request, message=f"Creating assessment question paper: {assessment.name}")
            question_paper = await self.latex_renderer.generate_assessment_document(request=request, assessment_type=assesment_type, assessment_id=assessment_id, assessment=assessment)
            await logger.log_message(request=request, message=f"Completed creating assessment question paper: {assessment.name}, question paper url is : {question_paper}")

            assessment_data['question_paper'] = question_paper

            # Store in database
            assessment_id = self.mongo_driver.insert_one(
                collection=f"{assesment_type}_db", 
                document=assessment_data
            )
            await logger.log_message(request=request, message=f"Assessment created with id: {assessment_id}")

            return {
                "status": "success",
                "assessment_id": assessment_id
            }
        
        except HTTPException as http_e:
            raise http_e

        except Exception as e:
            await logger.log_message(request=request, message=f"Error creating assignment: {str(e)}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")

    async def list_assessments(self, request:Request, assessment_filter: ListAssessments, assesment_type: str = Path(..., title="The type of assessment"),current_user: dict = Depends(check_user_authorization)):
        """
        This takes the course id , year and month filter to create list of all available assignments in that course in given month and year
        """
        try:
            await logger.log_message(request=request, message=f"Authenticated user")

            # Define valid filters for assessments
            valid_filters = {"course_id", "year", "month"}  # Add other valid filters as needed

            # Check for invalid filters
            if hasattr(assessment_filter, 'filters') and assessment_filter.filters:
                invalid_filters = [f for f in assessment_filter.filters.keys() if f not in valid_filters]
                if invalid_filters:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Invalid filter name(s): {', '.join(invalid_filters)}"
                    )

            aggregation_pipeline = await self.query_generator.generate_assessment_list_query(request=request, query_params=assessment_filter)

            assessments_list = self.mongo_driver.run_assessments_list_aggregation(collection=f"{assesment_type}_db", pipeline=aggregation_pipeline)

            if not assessments_list:
                await logger.log_message(request=request, message=f"No assessments found for the given filters")
                return {
                    "status": "success",
                    "data": []
                }

            return {
                "status": "success",
                "data": assessments_list
            }
        
        except HTTPException as http_e:
            raise http_e

        except Exception as e:
            await logger.log_message(request=request, message=f"Error listing assessments: {str(e)}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")

    async def get_assessment_details(self, request:Request, assessment: AssessmentId, assesment_type: str = Path(..., title="The type of assessment"),current_user: dict = Depends(check_user_authorization)):
        """
        This gets the details of the assignment with given id
        """

        try:
            await logger.log_message(request=request, message=f"Authenticated user")

            assessment_id = assessment.assessment_id

            assessment_query = {
                "_id": ObjectId(assessment_id)
            }

            await logger.log_message(request=request, message=f"Getting assessment data with query {assessment_query}")
            assessment_data = self.mongo_driver.find_one(collection=f"{assesment_type}_db",query=assessment_query)

            if not assessment_data:
                await logger.log_message(request=request, message=f"No assessment data was found")
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No Data found for given assessment")

            assessment_data["id"] = str(assessment_data.pop('_id'))

            await logger.log_message(request=request, message=f"Creatig a list of question id objects")
            question_ids = []
            question_ids = [ObjectId(q['question_id']) for section in assessment_data['questions'] for q in section['questions']]
            await logger.log_message(request=request, message=f"Completed creation of list of question id objects")

            storage_instance = CloudStorage()
            signed_url = await storage_instance.create_signed_url(request=request, object_url=assessment_data['question_paper'])
            assessment_data['question_paper'] = signed_url

            questions_data_query = {
                "_id": {
                    "$in": question_ids
                }
            }

            course = assessment_data['course']

            await logger.log_message(request=request, message=f"Creating a course object for data extraction")
            course_instance = Course(request=request, course_id=course)

            collection_name = await course_instance.get_question_library_collection()

            await logger.log_message(request=request, message=f"Getting questions data from from course questions data collection {collection_name}. Query : {str(questions_data_query)}")
            
            questions_data = self.mongo_driver.find(collection=collection_name, query=questions_data_query)
            
            await logger.log_message(request=request, message=f"Got questions data")

            print(questions_data)

            questions_data_map = dict()

            await logger.log_message(request=request, message=f"Convert questions data id object to string")
            for question in questions_data:
                question["id"] = str(question.pop('_id'))
                questions_data_map[str(question['id'])] = question
            print(questions_data_map)

            await logger.log_message(request=request, message=f"Mapped questions data to ids")

            for section in assessment_data['questions']:
                await logger.log_message(request=request, message=f"Updating questions data for section")
                for i, question in enumerate(section['questions']):
                    await logger.log_message(request=request, message=f"Updating question data for question id {str(question)}")
                    question_data = questions_data_map[question['question_id']]
                    question_marks = question['marks']
                    # Update the original dictionary with new data
                    section['questions'][i].update(question_data)
                    section['questions'][i]['marks'] = question_marks

            await logger.log_message(request=request, message=f"Completed assessment data fetching")

            return {
                "status": "success",
                "data": assessment_data
            }
        
        except HTTPException as http_e:
            raise http_e

        except Exception as e:
            await logger.log_message(request=request, message=f"Error fetching assessment details: {str(e)}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")

    async def generate_assessment(self, request:Request, query_params: Assessment, assesment_type: str = Path(..., title="The type of assessment"),current_user: dict = Depends(check_user_authorization)):
        return current_user

    async def edit_assessment(self, request:Request, assessment: EditAssessment, assesment_type: str = Path(..., title="The type of assessment"), current_user: dict = Depends(check_user_authorization)):
        """
        This takes assessment id and updated data to update a existing assessment
        """
        try:
            await logger.log_message(request=request, message=f"Authenticated user")

            # Unpacking QueryParams
            assessment_id = assessment.assessment_id
            assessment_data = {
                "name": assessment.name,
                "course": assessment.course,
                "start_date": assessment.start_date,
                "end_date": assessment.end_date,
                "total_time": assessment.total_time,
                "total_marks": assessment.total_marks,
                "lessons": assessment.lessons,
                "questions": [
                    {
                        "section_name": section.section_name,
                        "description": section.description,
                        "questions": [
                            {
                                "question_id": question.question_id,
                                "question_latex": question.question_latex,
                                "marks": question.marks
                            }
                            for question in section.questions
                        ]
                    }
                    for section in assessment.questions
                ],
                "last_updated": datetime.utcnow()
            }

            # Generate new question paper
            question_paper = await self.latex_renderer.generate_assessment_document(
                request=request,
                assessment_type=assesment_type,
                assessment_id=assessment_id,
                assessment=assessment
            )
            assessment_data["question_paper"] = question_paper

            obj_id = ObjectId(assessment_id)
            query = {"_id": obj_id}

            stat = self.mongo_driver.update_one(
                collection=f"{assesment_type}_db",
                query=query,
                update_data=assessment_data
            )

            return {
                "status": "success",
                "assessment_id": assessment_id
            }
        
        except HTTPException as http_e:
            raise http_e

        except Exception as e:
            await logger.log_message(request=request, message=f"Error editing assignment: {str(e)}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")

    async def grade_assessment(self, request:Request, grades: AssessmentMarks, assesment_type: str = Path(..., title="The type of assessment"),current_user: dict = Depends(check_user_authorization)):
        """
        This takes stundet grades for each assessment and updated to respective database collection
        """
        try:
            await logger.log_message(request=request, message=f"Authenticated user")

            assessment_id = grades.assessment_id

            course = grades.course

            student_grades = [{"assessment_id": assessment_id, "course_id": course, **grade.model_dump() } for grade in grades.marks]

            course_instance = Course(request=request, course_id=course)
            collection_name = await course_instance.get_question_library_collection()

            bulk_upload_query = await self.query_generator.generate_assessment_grading_query(request=request, grades=student_grades)

            result = self.mongo_driver.bulk_write(collection=f"{collection_name}_{assesment_type}_db",operations=bulk_upload_query)

            await logger.log_message(request=request, message=f"Updated assessment data to database collection {assesment_type}_db. Updated documents list {str(result)}")

            return {
                "status": "success",
                "data": result
            }

        except HTTPException as http_e:
            raise http_e

        except Exception as e:
            await logger.log_message(request=request, message=f"Error updating assessment data to database: {str(e)}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update assessment data to database")
    
    async def get_assessment_student_grades(self, request:Request, assessment_course: AssessmentCourse, assesment_type: str = Path(..., title="The type of assessment"),current_user: dict = Depends(check_user_authorization)):
        try:
            await logger.log_message(request=request, message=f"Authenticated user")
            assessment_id = assessment_course.assessment_id
            course = assessment_course.course

            await logger.log_message(request=request, message=f"Creating course object for course {course}")
            course_instance = Course(request=request, course_id=course)
            await logger.log_message(request=request, message=f"Getting course collection name")
            collection_name = await course_instance.get_question_library_collection()
            await logger.log_message(request=request, message=f"Getting course students data from MongoDB")
            students_data = await course_instance.get_course_students()

            students_mapping = {student['student_id']: student for student in students_data}

            query = {
                "assessment_id": assessment_id,
                "course_id": course
            }
            await logger.log_message(request=request, message=f"Getting course student grades data from MongoDB")
            student_grades = self.mongo_driver.find(collection=f"{collection_name}_{assesment_type}_db", query=query)

            if not student_grades:
                await logger.log_message(request=request, message=f"No student grades found for the given assessment and course")
                return {
                    "status": "success",
                    "data": [
                        {
                            "assessment_id": assessment_id,
                            "course_id": course,
                            **student,
                            "marks": None,
                            "status": None,
                        }
                        for student in students_data
                    ]
                }

            await logger.log_message(request=request, message=f"Fetched course student grades data from MongoDB")


            for student_grade in student_grades:
                student_grade.pop('_id')
                student_id = student_grade["student_id"]
                if student_id in students_mapping:
                    del students_mapping[student_id] 

            for missing_student in students_mapping.keys():
                student_grades.append({
                    "assessment_id": assessment_id,
                    "course_id": course,
                    **students_mapping[missing_student],
                    "marks": None,
                    "status": None,
                })

            return {
                "status": "success",
                "data": student_grades
            }

        except HTTPException as http_e:
            raise http_e

        except Exception as e:
            await logger.log_message(request=request, message=f"Error fetching student grades: {str(e)}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to fetch student grades")
        
    async def delete_assessment(self, request:Request, course_assessment: AssessmentCourse, assesment_type: str = Path(..., title="The type of assessment"),current_user: dict = Depends(check_user_authorization)):
        """
        This deletes an assessment and associated student grade documents based on the assessment id and course.
        """
        try:
            await logger.log_message(request=request, message=f"Attempting to delete assessment with id: {course_assessment.assessment_id}")

            assessment_id = course_assessment.assessment_id
            course = course_assessment.course

            # Delete the assessment document
            query = {"_id": ObjectId(assessment_id)}
            await logger.log_message(request=request, message=f"Deleting assessment using query : {str(query)}")
            result = self.mongo_driver.delete_one(collection=f"{assesment_type}_db", query=query)

            if result["deleted_count"] == 0:
                await logger.log_message(request=request, message=f"Assessment with id: {assessment_id} not found")
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Assessment not found")

            await logger.log_message(request=request, message=f"Deleted assessment document with id: {assessment_id}")

            # Delete associated student grade documents
            await logger.log_message(request=request, message=f"Creating course object for course {course}")
            course_instance = Course(request=request, course_id=course)
            await logger.log_message(request=request, message=f"Getting course collection name")
            collection_name = await course_instance.get_question_library_collection()

            
            try:
                await logger.log_message(request=request, message=f"Starting to delete student grade documents")

                query = {
                    "assessment_id": assessment_id,
                    "course_id": course
                }
                result = self.mongo_driver.delete_many(collection=f"{collection_name}_{assesment_type}_db", query=query)

                await logger.log_message(request=request, message=f"Deleted {result['deleted_count']} student grade documents")
            except Exception as e:
                await logger.log_message(request=request, message=f"Error deleting student grade documents: {str(e)}")

            return {
                "status": "success",
                "message": f"Assessment and associated {result['deleted_count']} student grade documents deleted successfully"
            }

        except HTTPException as http_e:
            await logger.log_message(request=request, message=f"HTTP Exception occurred while deleting assessment: {str(http_e)}")
            raise http_e

        except Exception as e:
            await logger.log_message(request=request, message=f"Error deleting assessment: {str(e)}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to delete assessment")

    