if (( $# == 2 ))
then
    python3 20171083_1.py $1 $2  > '20171083_1.txt'
else
    python3 20171083_2.py $1  > '20171083_2.txt'
fi
