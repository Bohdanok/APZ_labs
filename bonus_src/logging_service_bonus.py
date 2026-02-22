import grpc
from concurrent import futures
import logging_pb2
import logging_pb2_grpc

message_map = {}

class LoggingServiceServicer(logging_pb2_grpc.LoggingServiceServicer):
    def LogMessage(self, request, context):
        if request.uuid not in message_map:
            message_map[request.uuid] = request.msg
            print(f"[gRPC] Збережено: {request.msg} (UUID: {request.uuid})")
        else:
            print(f"[gRPC] Дублікат проігноровано (UUID: {request.uuid})")
        return logging_pb2.LogResponse(status="success")

    def GetMessages(self, request, context):
        msgs_str = ", ".join(message_map.values())
        return logging_pb2.MessagesResponse(messages=msgs_str)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    logging_pb2_grpc.add_LoggingServiceServicer_to_server(LoggingServiceServicer(), server)
    server.add_insecure_port('[::]:50051')
    print("gRPC Logging Service запущено на порту 50051...")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
