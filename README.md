# FindTheTarget
Proje Kapsamı
Bu proje, 1000x1000 boyutunda bir alan üzerinde iki adet sensör ve bir adet hedef konumlandıran bir kestirimle hedefi bulma uygulamasıdır. Sensörlerden alınan verileri işleyen merkezi birim, hedefin koordinat sistemine göre konumlandığı noktayı hesaplar ve görselleştirir.

Uygulama, Java programlama dili kullanılarak geliştirilmiştir. Veri akışını sağlamak için Apache Kafka mesajlaşma sistemi kullanılmıştır. Veritabanı olarak ise herhangi bir veritabanı kullanılmamıştır.

Kullanılan Teknolojiler
Java: Uygulama, Java programlama dili kullanılarak geliştirilmiştir. Java sürümü olarak Java Development Kit (JDK) 11 kullanılmıştır.
Apache Kafka: Veri akışını sağlamak için Apache Kafka mesajlaşma sistemi kullanılmıştır. Kafka, ana hatlarıyla olay akışı işleme (event streaming) sağlayan açık kaynaklı bir veri işleme platformudur. Kafka sürümü olarak 2.8.0 kullanılmıştır.
Gradle: Uygulamanın derlenmesi ve bağımlılıklarının yönetimi için Gradle kullanılmıştır. Gradle, açık kaynaklı bir proje yapısı yönetim aracıdır. Gradle sürümü olarak 7.0.2 kullanılmıştır.
IntelliJ IDEA: Uygulama geliştirme için IntelliJ IDEA kullanılmıştır. IntelliJ IDEA, açık kaynaklı bir Java entegre geliştirme ortamıdır.
Uygulamanın Derlenmesi ve Çalıştırılması
Uygulamanın derlenmesi ve çalıştırılması için aşağıdaki adımlar izlenmelidir:

1- Öncelikle, sisteminizde JDK 11 ve Gradle 7.0.2 yüklü olmalıdır. Ayrıca, Apache Kafka 2.8.0 sürümü indirilmelidir.

2- Proje klasörüne giderek, gradlew dosyasını çalıştırın. Bu, gerekli bağımlılıkların indirilmesini ve uygulamanın derlenmesini sağlar.

cd <proje_klasörü>
./gradlew build
3- Kafka'yı çalıştırın. Kafka'yı çalıştırmadan önce, server.properties dosyasını açın ve advertised.listeners özelliğini PLAINTEXT://localhost:9092 olarak ayarlayın.

cd <kafka_klasörü>
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties

4- Uygulamayı çalıştırın.
