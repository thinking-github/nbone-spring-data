#!/bin/bash
# mvn  -U -Dmaven.test.skip=true package deploy -Prelease  -X
mvn -Dmaven.test.skip=true deploy