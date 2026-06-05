#!/bin/bash

# Proto code generation script for eventbus
# Uses buf as a modern alternative to protoc
# Usage: ./gen_proto.sh

set -e

echo "Checking buf installation..."
if ! command -v buf &> /dev/null; then
    echo "buf is not installed. Installing..."
    go install github.com/bufbuild/buf/cmd/buf@latest
fi

echo "Installing protoc-gen-go plugin..."
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

echo "Generating Go code from proto files using buf..."

# Generate proto files
buf generate

echo "Proto code generation completed!"
echo "Generated files:"
ls -la proto/*.pb.go 2>/dev/null || echo "No .pb.go files found in proto/ directory"

echo ""
echo "Note: If buf is not available, you can manually install protoc:"
echo "  Ubuntu/Debian: sudo apt-get install protobuf-compiler"
echo "  macOS: brew install protobuf"
echo "  Or download from: https://github.com/protocolbuffers/protobuf/releases"