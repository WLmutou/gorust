#!/usr/bin/env bash

wrk -t4 -c100 -d10s http://127.0.0.1:8080
