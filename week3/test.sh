#!/bin/bash

head -n 1 /srv/home/rgai0001/week3/SalesJan2009.csv > header.txt
awk -F, '{
         for(i==1; i<=NF; i++) {
            if(i==1) {printf "HBASE_ROW_KEY,"}
            else if(i==NF) {printf "col:%s", $i}
            else {printf "col:%s,", $i};
          }
          print NL
}' header.txt
