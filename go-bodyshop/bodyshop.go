package main

import (
	"context"
	"github.com/tech-greedy/singularity/go-bodyshop/util"
	"io"
	"os"
)

func main() {
	ctx := context.Background()
	fileList := []util.Finfo{
		{
			Path:      "",
			Name:      "",
			Info:      nil,
			SeekStart: 0,
			SeekEnd:   0,
		},
	}

	parentPath := ""
	parallel := 1


	carF, err := os.Create("test.car")
	if err != nil {
		panic(err)
	}
	defer carF.Close()

	piper, pipew := io.Pipe()
	go func() {
		util.CalculateCommp(piper)
	}()

	writer := io.MultiWriter(carF, pipew)
	defer carF.Close()
	defer pipew.Close()
	util.GenerateCar(ctx, fileList, parentPath, writer, parallel)
}