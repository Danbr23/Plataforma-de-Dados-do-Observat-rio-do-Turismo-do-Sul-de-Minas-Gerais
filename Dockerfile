FROM python:3.12-slim

WORKDIR /app

# The installer requires curl (and certificates) to download the release archive
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates

# Download the latest installer
ADD https://astral.sh/uv/install.sh /uv-installer.sh

# Run the installer then remove it
RUN sh /uv-installer.sh && rm /uv-installer.sh

# Ensure the installed binary is on the `PATH`
ENV PATH="/root/.local/bin/:$PATH"

COPY pyproject.toml uv.lock .python-version ./

RUN uv sync

COPY . .

# Set environment variables for Django
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

EXPOSE 8000

# # Run Django and start the development server
#CMD ["uv", "run", "otsuldeminas/manage.py", "runserver", "0.0.0.0:8000"]