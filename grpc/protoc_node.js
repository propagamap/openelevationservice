const fs = require('fs');
const path = require('path');
const childProcess  = require('child_process');

if (process.argv.length < 4) {
    console.error('Usage: node protoc_node.js <proto_path> <output_path>');
    process.exit(1);
}

const distPath = path.join(__dirname, process.argv[2]);
const protoPath = path.join(__dirname, process.argv[3]);
const isWeb = process.argv.includes('--web');

if (!fs.existsSync(distPath)) {
  fs.mkdirSync(distPath, { recursive: true });
}

const protoDefs = fs.readdirSync(protoPath).filter((file) => file.endsWith('.proto'));

let command = ''
if (isWeb) {
  command = `npm run __protoc -- --js_out=import_style=commonjs:${distPath}/ --grpc-web_out=import_style=commonjs+dts,mode=grpcwebtext:${distPath}/ --plugin=protoc-gen-grpc=protoc-gen-grpc-web -I ${protoPath}/ ${protoDefs.join(' ')}`;
} else {
  command = `npm run __protoc -- --js_out=import_style=commonjs,binary:${distPath}/ --ts_out=grpc_js:${distPath}/ --grpc_out=grpc_js:${distPath}/ -I ${protoPath}/ ${protoDefs.join(' ')}`;
}

childProcess.exec(command, (err, stdout) => {
  if (err) {
    console.log(err);
    return;
  }
  console.log(stdout);

  const services = protoDefs.map((protoDef) => protoDef.substring(0, protoDef.lastIndexOf('.')));

  const fileSuffix = isWeb ? '_grpc_web_pb' : '_grpc_pb';
  const indexDTSContent = services.map((serviceName) => `export * from './${serviceName}${fileSuffix}';\nexport * from './${serviceName}_pb';\n`).join('');
  fs.writeFileSync(path.join(distPath, 'index.d.ts'), indexDTSContent);

  const indexJSImports = services.map((serviceName) => `  ...require('./${serviceName}${fileSuffix}'),\n  ...require('./${serviceName}_pb')`).join(',\n');
  const indexJSContent = 'module.exports = {\n' + indexJSImports + '\n};\n';
  fs.writeFileSync(path.join(distPath, 'index.js'), indexJSContent);
});