import json
import logging
import time
from logging.handlers import TimedRotatingFileHandler

handler = TimedRotatingFileHandler(
    "grpc_access.log", when="midnight", backupCount=3, encoding="utf-8"
)
handler.setFormatter(logging.Formatter("%(message)s"))

logger = logging.getLogger("grpc_logger")
logger.setLevel(logging.INFO)
logger.addHandler(handler)
logger.propagate = False  # avoid duplicate logs if root logger is used elsewhere

try:
    # generalresearch should NOT have a grpc dependency, so put
    #   this whole thing in a try-catch..
    import grpc

    class LoggingInterceptor(grpc.ServerInterceptor):
        def intercept_service(self, continuation, handler_call_details):
            method = handler_call_details.method
            handler = continuation(handler_call_details)

            if handler is None:
                return None

            def log_and_call(handler_func, request, context):
                start_time = time.time()
                code = grpc.StatusCode.INTERNAL
                try:
                    response = handler_func(request, context)
                    code = context.code() or grpc.StatusCode.OK
                    return response
                except Exception as e:
                    code = context.code() or grpc.StatusCode.INTERNAL
                    raise e
                finally:
                    duration_ms = int((time.time() - start_time) * 1000)
                    peer = context.peer() or "unknown"
                    logger.info(
                        json.dumps(
                            {
                                "method": method,
                                "code_value": code.value[0],
                                "code_name": code.value[1],
                                "duration": duration_ms,
                                "peer": peer,
                                "time": start_time,
                            }
                        )
                    )

            if handler.unary_unary:
                return grpc.unary_unary_rpc_method_handler(
                    lambda request, context: log_and_call(
                        handler.unary_unary, request, context
                    ),
                    request_deserializer=handler.request_deserializer,
                    response_serializer=handler.response_serializer,
                )

            elif handler.unary_stream:
                return grpc.unary_stream_rpc_method_handler(
                    lambda request, context: log_and_call(
                        handler.unary_stream, request, context
                    ),
                    request_deserializer=handler.request_deserializer,
                    response_serializer=handler.response_serializer,
                )

            else:
                return handler

except ImportError as e:
    print(e)
    LoggingInterceptor = None
