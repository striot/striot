#!/bin/bash
set -u
set -e
shopt -s nullglob

# Generates a Makefile with a default target that performs the following steps
# for every applicable example under examples/:
#
#   • build and run "generate" (clean first to ensure it's regenerated)
#   • build any node?/node.hs files output by "generate"
#
# This can be used as a brute-force method of ensuring that the above all
# typecheck and compile properly when making structural changes to the Striot
# library (and is in lieu of a better test suite)

gen()
{
    a=()
    echo default: default2
    echo
    for m in examples/*/generate.hs; do
        d="$(dirname "$m")"
        a+=("$d")

        echo -e "$d:"
        echo -e "\tmake -C $d clean"
        echo -e "\tmake -C $d generate"
        echo -e "\tcd \"$d\" && ./generate"

        for n in "$d"/node?/node.hs
            do echo -e "\tghc -i. $n"
        done
        echo
    done

    echo clean:
    for d in "${a[@]}"; do
        echo -e "\tmake -C $d clean"
    done
    echo

    echo default2: "${a[@]}"
    echo .PHONY: default default2 clean "${a[@]}"
}

gen
