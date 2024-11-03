# Stage 1: Build the application
FROM rust:alpine as builder
WORKDIR /usr/src/app
RUN apk add pkgconfig openssl-dev libc-dev
COPY . .
RUN cargo build --release

# Stage 2: Create the scratch image
FROM scratch
EXPOSE 8080
COPY --from=builder /usr/src/app/target/release/personsvc /personsvc
ENTRYPOINT ["/personsvc"]
