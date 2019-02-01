#!/usr/bin/env bash


###########################################################
## Common content with bin/run-docker.
## Use that version as reference.

function find-latest-mini-local {
    name=zppz/mini

    tag=$(docker images "${name}" --format "{{.Tag}}" | sort | tail -n 1)
    if [[ "${tag}" == '' ]]; then
        echo -
    else
        echo "${name}:${tag}"
    fi
}


function find-latest-mini {
    name=zppz/mini

    local=$(find-latest-mini-local)

    url=https://hub.docker.com/v2/repositories/${name}/tags
    tags="$(curl -L -s ${url} | tr -d '{}[]"' | tr ',' '\n' | grep name)"
    if [[ "$tags" == "" ]]; then
        remote=-
    else
        tags="$(echo $tags | sed 's/name: //g' | sed 's/results: //g')"
        tag=$(echo "${tags}" | tr ' ' '\n' | sort -r | head -n 1)
        remote="${name}:${tag}"
    fi

    if [[ "${local}" == - ]]; then
        echo "${remote}"
    elif [[ "${remote}" == - ]]; then
        echo "${local}"
    elif [[ "${local}" < "${remote}" ]]; then
        echo "${remote}"
    else
        echo "${local}"
    fi
}


## End of common content.
#############################


IMG=$(find-latest-mini) || exit 1
if [[ "${IMG}" == - ]]; then
    echo "Unable to find image 'mini'"
    exit 1
fi

cmd="$(docker run --rm ${IMG} /usr/local/bin/make-proj-builder)"
bash -c "${cmd}" -- $@
#docker run --rm ${IMG} /usr/local/bin/make-proj-builder
