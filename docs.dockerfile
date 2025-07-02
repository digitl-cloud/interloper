FROM python:3.10-slim
RUN pip install mkdocs-material
WORKDIR /app
COPY mkdocs.yml .
COPY docs docs
EXPOSE 8000
CMD ["mkdocs", "serve", "--dev-addr=0.0.0.0:8000"]