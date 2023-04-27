class ErrorHandler(Exception):
    pass


class CourseNotFoundError(ErrorHandler):
    pass


class InvalidCourseKeyError(ErrorHandler):
    pass


class TimeKeyError(ErrorHandler):
    pass


class NotAuthorizedError(ErrorHandler):
    pass
