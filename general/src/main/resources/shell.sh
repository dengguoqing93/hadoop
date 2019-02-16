#!/usr/bin/env bash

#写入SequenceFile对象
hadoop jar general-1.0-SNAPSHOT.jar io.SequenceFileWriteDemo numbers.seq
#读取SequenceFile对象
hadoop jar general-1.0-SNAPSHOT.jar io.SequenceFileReadDemo numbers.seq