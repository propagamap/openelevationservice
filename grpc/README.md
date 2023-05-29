# OES gRPC definitions

## Linux

### Regenerate Python server definitions

Run the following commands in the **root folder** of this repository.

- Install additional dependencies:

```bash
pip3 install grpcio-tools
```

- Build definition files:

```bash
python3 -m grpc_tools.protoc -I ./grpc/proto --python_out=. --pyi_out=. --grpc_python_out=. ./grpc/proto/openelevationservice/server/grpc/openelevation.proto
```

### Generate Node.js client package

Run the following commands in this **grpc** folder.

- Install dependencies:

```bash
yarn install
```

- Build definition files:

```bash
yarn generate
```

- Publish package:

```bash
npm publish
```

## Windows

### Regenerate Python server definitions

Run the following commands in the **root folder** of this repository.

- Install additional dependencies:

```bash
pip3 install grpcio-tools
```

- Build definition files:

```bash
python -m grpc_tools.protoc -I ./grpc/proto --python_out=. --pyi_out=. --grpc_python_out=. ./grpc/proto/openelevationservice/server/grpc/openelevation.proto
```

### Generate Node.js client package

Run the following commands in this **grpc** folder.

- Install dependencies:

```bash
yarn install
```

- Build definition files:

```bash
yarn generate:win
```

- Publish package:

```bash
npm publish
```
