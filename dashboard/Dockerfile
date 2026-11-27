# Stage 1: build
FROM node:18-alpine AS builder
WORKDIR /app
COPY package.json .
RUN npm install --legacy-peer-deps
# Fix: react-scripts 5 + node 18 ajv peer dep conflict
RUN npm install --legacy-peer-deps ajv@^8 ajv-keywords@^5
COPY . .
RUN npm run build

# Stage 2: serve with nginx
FROM nginx:alpine
COPY --from=builder /app/build /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
