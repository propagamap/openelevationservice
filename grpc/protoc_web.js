const fs = require('fs');
const path = require('path');
const subprocess = require('child_process');

if (process.argv.length !== 4) {
  console.error('Invalid number of arguments\nUsage: node protoc_node.js <proto-src> <node-dist>\n');
  process.exit(1);
}

const protoPath = path.join(__dirname, process.argv[2]);
const distPath = path.join(__dirname, process.argv[3]);

if (!fs.existsSync(distPath)) {
  fs.mkdirSync(distPath, { recursive: true });
}

const protoDefs = fs.readdirSync(protoPath).filter((file) => file.endsWith('.proto'));

if (protoDefs.length === 0) {
  console.error(`No proto files found in "${protoPath}"\n`);
  process.exit(2);
}

const command = `npm run __protoc -- --js_out=import_style=commonjs,binary:${distPath} --ts_out=grpc_js:${distPath} --grpc_out=grpc_js:${distPath} -I ${protoPath} ${protoDefs.join(' ')}`;

subprocess.exec(command, (err, stdout) => {
  if (err) {
    console.log(err);
    process.exit(3);
  }
  console.log(stdout);

  const services = protoDefs.map((protoDef) => protoDef.substring(0, protoDef.lastIndexOf('.')));

  const indexDTSContent = services.map((serviceName) => `export * from './${serviceName}_grpc_pb';\nexport * from './${serviceName}_pb';\n`).join('');
  fs.writeFileSync(path.join(distPath, 'index.d.ts'), indexDTSContent);

  const indexJSImports = services.map((serviceName) => `  ...require('./${serviceName}_grpc_pb'),\n  ...require('./${serviceName}_pb')`).join(',\n');
  const indexJSContent = 'module.exports = {\n' + indexJSImports + '\n};\n';
  fs.writeFileSync(path.join(distPath, 'index.js'), indexJSContent);
});
