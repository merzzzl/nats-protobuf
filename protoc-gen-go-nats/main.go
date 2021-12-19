package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strings"
	"text/template"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

func templateFunc() template.FuncMap {
	return template.FuncMap{
		"lower": func(s string) string {
			return strings.ToLower(s)
		},
		"upper": func(s string) string {
			return strings.ToUpper(s)
		},
		"snake": func(s string) string {
			rx := regexp.MustCompile(`(.)([A-Z])`)
			snake := rx.ReplaceAllString(s, "${1}_${2}")
			return strings.ToUpper(snake)
		},
		"base": func(in string) string {
			idx := strings.LastIndex(in, ".")
			if idx == -1 {
				return in
			}
			return in[idx+1:]
		},
		"package": func(s string) string {
			sp := strings.Split(s, ";")
			return sp[len(sp)-1]
		},
	}
}

func goFileName(f *descriptorpb.FileDescriptorProto) string {
	name := "service"
	if f.Name != nil {
		fname := []string{*f.Name}
		fname = strings.Split(fname[0], ".proto")
		fname = strings.Split(fname[0], "/")
		name = fname[len(fname)-1]
	}
	sp := strings.Split(f.Options.GetGoPackage(), ";")
	return fmt.Sprintf("%s/%s.pb.nats.go", sp[0], name)
}

func main() {
	log.SetPrefix("protoc-gen-go-nats: ")
	data, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		log.Fatalf("error: reading input:  %v", err)
	}

	var request pluginpb.CodeGeneratorRequest
	err = proto.Unmarshal(data, &request)
	if err != nil {
		log.Fatalf("error: parsing input proto: %v", err)
	}

	tmpl, err := template.New(".").Funcs(templateFunc()).Parse(tmpl)
	if err != nil {
		log.Fatalf("error: parsing tmpl: %v", err)
	}

	response := &pluginpb.CodeGeneratorResponse{}
	for _, file := range request.ProtoFile {
		if len(file.Service) == 0 {
			continue
		}

		if file.Options == nil || file.Options.GoPackage == nil {
			log.Fatalf("error: proto options: %v", fmt.Errorf("missing go_package"))
		}

		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, file); err != nil {
			log.Fatalf("error: execute tmpl: %v", err)
		}

		file := &pluginpb.CodeGeneratorResponse_File{
			Name:    proto.String(goFileName(file)),
			Content: proto.String(buf.String()),
		}

		response.File = append(response.File, file)
	}

	data, err = proto.Marshal(response)
	if err != nil {
		log.Fatalf("error: failed to marshal output proto: %v", err)
	}

	_, err = os.Stdout.Write(data)
	if err != nil {
		log.Fatalf("error: failed to write output proto: %v", err)
	}
}
