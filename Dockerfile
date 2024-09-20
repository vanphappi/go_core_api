FROM golang:latest

# Move to working directory (/app).
WORKDIR /app

# Copy and download dependency using go mod.
COPY go.mod go.sum ./
RUN go mod download

# Copy the code into the container.
COPY . .

# 
RUN go build -ldflags="-s -w" -o ./build/execute .
