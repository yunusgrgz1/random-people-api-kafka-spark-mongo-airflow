# OpenJDK 17 tabanlı bir image kullanıyoruz
FROM openjdk:17-jdk-slim

# Java uygulamalarını konteynıra yüklemek için bir dizin oluşturuyoruz
WORKDIR /opt/java

# (Opsiyonel) Uygulamanızın tüm dosyalarını konteynıra kopyalayabilirsiniz
#COPY . .

# Java 17 ile ilgili temel komutu çalıştırarak doğrulama yapıyoruz
CMD ["java", "-version"]
