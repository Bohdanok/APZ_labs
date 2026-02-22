import grpc
import logging_pb2
import logging_pb2_grpc

def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = logging_pb2_grpc.LoggingServiceStub(channel)
        print("Відправка 1...")
        stub.LogMessage(logging_pb2.LogRequest(uuid="duplicate-uuid-777", msg="Hello"))
        print("Відправка 2 (дублікат)...")
        stub.LogMessage(logging_pb2.LogRequest(uuid="duplicate-uuid-777", msg="Hello again"))

if __name__ == '__main__':
    run()