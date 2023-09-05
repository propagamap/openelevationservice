# OES gRPC definitions

The additional dependencies are also required to run the server in gRPC mode.

## Linux

### Regenerate Python server definitions

Run the following commands in the **root folder** of this repository.

- Install additional dependencies:

```bash
pip3 install grpcio-tools
pip3 install grpcio-reflection
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
yarn build
```

- (For the first time only on each machine) Configure npm for this folder following the instructions in the [organization documentation](https://github.com/propagamap/docs/wiki/GitHub#publish-package-cheatsheet).

- Publish package:

```bash
yarn version --patch

npm publish
```

## Windows

### Regenerate Python server definitions

Run the following commands in the **root folder** of this repository.

- Install additional dependencies:

```bash
pip3 install grpcio-tools
pip3 install grpcio-reflection
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
yarn build:win
```

- (For the first time only on each machine) Configure npm for this folder following the instructions in the [organization documentation](https://github.com/propagamap/docs/wiki/GitHub#publish-package-cheatsheet).

- Publish package:

```bash
yarn version --patch

npm publish
```
