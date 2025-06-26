# OES gRPC definitions

The additional dependencies are also required to run the server in gRPC mode.

## Regenerate Python server definitions

Run the following commands in the **root folder** of this repository.

- Install additional dependencies:

```bash
pip3 install grpcio-tools
pip3 install grpcio-reflection
```

- Build definition files:

```bash
# Linux
python3 -m grpc_tools.protoc -I ./grpc/proto --python_out=. --pyi_out=. --grpc_python_out=. ./grpc/proto/openelevationservice/server/grpc/openelevation.proto

# Windows
python -m grpc_tools.protoc -I ./grpc/proto --python_out=. --pyi_out=. --grpc_python_out=. ./grpc/proto/openelevationservice/server/grpc/openelevation.proto
```

## Generate Web and Node.js client package

For gRPC-Web Clients

- Generates types using @propagamap/oes-grpc-web as package name

For Node.js Clients

- Edit grpc/package.json and change:
        
            diff
            - "name": "@propagamap/oes-grpc-web"
            + "name": "@propagamap/oes-grpc-ts"
  
- This will allow you to generate types using @propagamap/oes-grpc-ts as the package name.

-[Edit package.json](./grpc/package.json) 


Run the following commands in this **grpc** folder.

- Install dependencies:

```bash
yarn install
```

Build definition files:

- Web Version

```bash
yarn build-web
```

- Node.js Version

```bash
yarn build
```

- (For the first time only on each machine) Configure npm for this folder following the instructions in the [organization documentation](https://github.com/propagamap/docs/wiki/GitHub#publish-package-cheatsheet).

- Publish package:

```bash
yarn version --patch

npm publish
```