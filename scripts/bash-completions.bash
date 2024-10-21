#/usr/bin/env bash
# Bash completions for spectral-line-imaging-pipeline

PIPELINE_OPT="run install-config diagnose"
RUN_OPT="--help --input --config --output --stages --dask-scheduler --with-report --verbose"
CONFIG_OPT="--help --config-install-path --set"
DIAGNOSE_OPT="--help --input --channel --output --dask-scheduler"

function _install_config_cmd() {
    local curr prev
    prev=$1
    curr=$2
    COMPREPLY=($(compgen -W "$CONFIG_OPT" -- ${cur}))

    case ${prev} in
        --set )
            keys=($(yq --output-format props $YAML_PATH/spectral_line_imaging_pipeline.yml | sed 's/^//; s/ =.*$//g'));
            COMPREPLY=( $(egrep -o "${curr//./\\.}[^ ]+" <<< ${keys[@]}) );
            ;;
        --config-install-path )
            COMPREPLY=()
    esac
}

_sip_completions() {
  local cur prev

  subcmd=${COMP_WORDS[1]}
  cur=${COMP_WORDS[COMP_CWORD]}
  prev=${COMP_WORDS[COMP_CWORD-1]}

  case ${COMP_CWORD} in
    1)
      COMPREPLY=($(compgen -W "$PIPELINE_OPT" -- ${cur}))
      ;;
    *)
      case ${subcmd} in
        run)
          if [[ ${cur} == -* ]] ; then
            COMPREPLY=($(compgen -W "$RUN_OPT" -- ${cur}))
          fi
          ;;
        install-config)
          _install_config_cmd $prev $cur
          ;;
        diagnose)
          if [[ ${cur} == -* ]] ; then
            COMPREPLY=($(compgen -W "$DIAGNOSE_OPT" -- ${cur}))
          fi
          ;;
        esac
        ;;
      esac
}

complete -o bashdefault -o default -o nospace -F _sip_completions spectral-line-imaging-pipeline
