#!/bin/sh

if [ -z "${BENCHMARK_HOME}" ]; then
  export BENCHMARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

echo $BENCHMARK_HOME

BENCHMARK_CONF="$BENCHMARK_HOME"/conf/config.properties
echo $BENCHMARK_CONF

MAIN_CLASS=cn.edu.tsinghua.iotdb.benchmark.App

CLASSPATH="."
for f in ${BENCHMARK_HOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done


if [ -n "$JAVA_HOME" ]; then
    for java in "$JAVA_HOME"/bin/amd64/java "$JAVA_HOME"/bin/java; do
        if [ -x "$java" ]; then
            JAVA="$java"
            break
        fi
    done
else
    JAVA=java
fi

cd $BENCHMARK_HOME
exec "$JAVA" -Duser.timezone=GMT+8 -Dlogback.configurationFile=${BENCHMARK_HOME}/conf/logback.xml  -cp "$CLASSPATH" "$MAIN_CLASS" "$@" -cf "$BENCHMARK_CONF"

exit $?