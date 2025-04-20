#!/bin/sh

todos=$(git grep -n "TODO" -- ':!*.md' ':!*.ipynb' ':!*.sh' ':!test_*.py')

if [ -n "$todos" ]; then
    echo "WARNING: The following TODOs were found in the code:"
    echo "$todos"
    echo "Consider addressing these TODOs before merging."
    exit 1
else
    echo "No TODOs found."
    exit 0
fi
