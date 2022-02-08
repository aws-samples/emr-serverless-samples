FROM 895885662937.dkr.ecr.us-west-2.amazonaws.com/spark/emr-6.3.0:latest AS base

RUN touch main.py && \
    spark-submit --conf spark.jars.ivy=$HOME/.ivy-glow --packages io.projectglow:glow-spark3_2.12:1.1.2 main.py || echo "OK"

RUN ls -alFF /home/hadoop/.ivy-glow/jars/
RUN echo $HOME/.ivy-glow

FROM scratch AS export
COPY --from=base /home/hadoop/.ivy-glow/jars/*.jar /