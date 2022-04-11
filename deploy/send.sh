#!/bin/bash

. ./g5k.conf

SITE="$1.g5k"
FILE=$2

echo "send to $SITE the file $FILE, with $USERNAME"
scp $FILE "$USERNAME@access.grid5000.fr:$1/$FILE"

