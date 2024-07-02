import os, shutil, sys
from grpc_tools import protoc

PROJECT_ROOT = "."
PROTO_ROOT = os.path.join(".", "grpc", "proto")
PROTO_INFIX_PATH = os.path.join("openelevationservice", "server", "grpc")
PROTO_FILE = "openelevation.proto"


def generate_grpc(project_root=None, proto_infix_path=None) -> None:
    if project_root is None:
        project_root = PROJECT_ROOT
    if proto_infix_path is None:
        proto_infix_path = PROTO_INFIX_PATH

    proto_dir = os.path.join(PROTO_ROOT, proto_infix_path)
    proto_target = os.path.join(proto_dir, PROTO_FILE)

    if not os.path.exists(proto_target):
        original_proto = os.path.join(PROTO_ROOT, PROTO_INFIX_PATH, PROTO_FILE)

        os.makedirs(proto_dir, exist_ok=True)
        shutil.copyfile(original_proto, proto_target)

    return protoc.main(
        (
            "",
            f"--proto_path={PROTO_ROOT}",
            f"--python_out={project_root}",
            f"--pyi_out={project_root}",
            f"--grpc_python_out={project_root}",
            proto_target,
        )
    )


if __name__ == "__main__":
    if os.getcwd().endswith("grpc"):
        print("Execute this script in the root directory, not in the grpc directory.")
        sys.exit(1)
    if len(sys.argv) > 3:
        print(
            "Usage: python ./grpc/protoc_python.py [target_root] [target_path_under_root]"
        )
        sys.exit(2)

    project_root = sys.argv[1] if len(sys.argv) > 1 else PROJECT_ROOT
    proto_infix_path = sys.argv[2] if len(sys.argv) > 2 else PROTO_INFIX_PATH

    status = generate_grpc(project_root, proto_infix_path)
    if status != 0:
        print("protoc exited with status", status)
    sys.exit(status)
