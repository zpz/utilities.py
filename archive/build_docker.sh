#!/usr/bin/env bash


###########################################################
## Common content with bin/run-docker.
## Use that version as reference.

function find-latest-mini-local {
    local name=zppz/mini

    local tag=$(docker images "${name}" --format "{{.Tag}}" | grep -E "^[0-9]{8}T[0-9]{6}Z$" | sort | tail -n 1) || return 1
    if [[ "${tag}" == '' ]]; then
        echo -
    else
        echo "${name}:${tag}"
    fi
}


function find-latest-mini {
    local name=zppz/mini

    local tags
    local tag
    local localimg
    local remoteimg

    localimg=$(find-latest-mini-local) || return 1

    local url=https://hub.docker.com/v2/repositories/${name}/tags

    tags="$(curl -Ls ${url} | tr -d '{}[]"' | tr ',' '\n' | grep name)" || tags=''
    if [[ "$tags" == "" ]]; then
        remoteimg=-
    else
        tags="$(echo $tags | sed 's/name: //g' | sed 's/results: //g')" || return 1
        tag=$(echo "${tags}" | tr ' ' '\n' | grep -E "^[0-9]{8}T[0-9]{6}Z$" | sort | tail -n 1) || return 1
        remoteimg="${name}:${tag}"
    fi

    if [[ "${localimg}" == - ]]; then
        echo "${remoteimg}"
    elif [[ "${remoteimg}" == - ]]; then
        echo "${localimg}"
    elif [[ "${localimg}" < "${remoteimg}" ]]; then
        echo "${remoteimg}"
    else
        echo "${localimg}"
    fi
}


## End of common content.
#############################


IMG=$(find-latest-mini) || exit 1
if [[ "${IMG}" == - ]]; then
    echo "Unable to find image 'mini'"
    exit 1
fi

cmd="$(docker run --rm ${IMG} make-proj-builder)" || exit 1
timestamp=$(docker run --rm ${IMG} make-ts-tag)

name=zpz
parent=zppz/py3
bash -c "${cmd}" -- \
    --name ${name} \
    --parent ${parent} \
    --timestamp ${timestamp}
