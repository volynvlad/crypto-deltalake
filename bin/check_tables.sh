#!/bin/sh
du -h tables/ | grep -v "tables/$" | while read -r size path
do
    n_files=$(ls $path | wc -l)
    echo "Path: $path. Size $size, number of files $n_files"

    case "$path" in *"_delta_log"*)
        ;;
    *)
        if [ $(ls $path | grep "parquet" | wc -l) -gt 0 ]; then
            echo "\t$path used: $(ls $path | grep "zstd" | wc -l)"
            echo "\t$path unused: $(ls $path | grep "snappy" | wc -l)"
        fi
        ;;
    esac
done
