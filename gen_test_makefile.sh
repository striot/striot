#!/bin/bash
set -u
set -e
shopt -s nullglob

# Generates a Makefile with a default target that performs the following steps
# for every applicable example under examples/:
#
#  1. build and run "generate" (clean first to ensure it's regenerated)
#  2. build any node?/node.hs files output by "generate"
#
# In a clean checkout, this script needs to be run *twice*: the first Makefile
# to generate all the individual nodes, and the second run to generate rules 
# to build all of them.
#
# This can be used as a brute-force method of ensuring that the above all
# typecheck and compile properly when making structural changes to the Striot
# library (and is in lieu of a better test suite)

gen()
{
    a=()
    echo GHC := stack ghc -- -XTemplateHaskell
    echo default: default2
    echo
    for m in examples/*/generate.hs; do
        d="$(dirname "$m")"
        a+=("$d")

        echo -e "$d:"
        echo -e "\t+make -C $d clean GHC=\"\$(GHC)\""
        echo -e "\t+make -C $d generate GHC=\"\$(GHC)\""
        echo -e "\tcd \"$d\" && ./generate"

        for n in "$d"/node?/node.hs
            do echo -e "\t\$(GHC) -isrc -i$d $n"
        done
        echo
    done

    echo clean:
    for d in "${a[@]}"; do
        echo -e "\t+make -C $d clean GHC=\"\$(GHC)\""
    done
    echo

    echo default2: "${a[@]}"
    echo .PHONY: default default2 clean "${a[@]}"
}

gen
