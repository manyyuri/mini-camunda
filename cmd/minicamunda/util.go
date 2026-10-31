// 简单文件读取（避免 demo 依赖）。
package main

import "os"

func readFileBytes(p string) ([]byte, error) { return os.ReadFile(p) }
