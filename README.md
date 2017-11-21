# Big_data_kmeans

Big_data_kmeans es una práctica realizada para la materia de tópicos especiales en telemática en la Universidad EAFIT, la cual consiste en la realización de un algoritmo que permita clasificar textos a partir de su contenido.

El ejemplo de la ejecución de los algortimos del repositorio pueden ser probado con el dataset de gutemberg, el cúal se encuentra en el datacenter académico de la Universidad EAFIT en la dirección 192.168.10.75:8080

Podemos acceder por ssh de la siguiente manera 

    ssh csanch35@192.168.10.75

Luego 

Para ejecutar el programa usar el siguiente comando 

    $ spark-submit --master yarn --deploy-mode cluster [--executor-memory <memoria>] [--num-executors <procesadores>] kmeans.py <dataset> <salida> <k> <max iter>

    donde 
        <memoria> es la cantidad de memoria a ser usada por el programa (este parametro es opcional, con un valor de 1GB por defecto
        <procesadores> es la cantidad de procesadores a ser usados en la ejecucion (este parametro es opcional, con un valor de 2 por defecto)
        <dataset> es la ruta del dataset a ser utilizado
        <salida> es la ruta a ser usada para guardar la salida del algoritmo
        <k> se refiere al parametro k del algoritmo Kmeans
        <max iter> se refiere a la cantidad maxima de iteraciones que puede ejecutar el algoritmo
