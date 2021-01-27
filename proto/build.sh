#all:
#	protoc --gofast_out=. *.proto
#protoc -I=. -I=$GOPATH/src -I=$GOPATH/src/github.com/gogo/protobuf/proto --gogofaster_out=. *.proto
protoc -I=. --gogoslick_out=../../flyfish/proto *.proto