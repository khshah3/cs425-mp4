package logger

import (
    "log"
    "os"
    "bufio"
)

type Logger struct {
  key, value, filename string
}

func (logs *Logger) InitializeLogFileName(filename string){
  file, err := os.Open(filename)
  if err != nil {
    log.Fatal(err)
  }
  scanner := bufio.NewScanner(file)
  scanner.Scan()
  logs.filename = scanner.Text()
  file.Close()

}

func (logs *Logger) SetLogFileName(filename string) {
    logs.filename = filename
}

func (logs *Logger) GetLogFileName() (string) {
    return logs.filename
}

func (logs *Logger) FileLog(key string , value string)(int64){
    file, err := os.OpenFile(logs.filename, os.O_APPEND | os.O_CREATE | os.O_RDWR, 0666)
    defer file.Close()
    if err != nil {
        log.Fatal(err)
    }
    file.WriteString(key + ":" + value + "\n")
    fileLength , err := file.Seek(0, os.SEEK_CUR)
    return fileLength
}

func Log(key string , value string)(int64){
    file, err := os.OpenFile("logs/applications.log", os.O_APPEND | os.O_CREATE | os.O_RDWR, 0666)
    defer file.Close()
    if err != nil {
        log.Fatal(err)
    }
    file.WriteString(key + ":" + value + "\n")
    fileLength , err := file.Seek(0, os.SEEK_CUR)
    return fileLength
}


