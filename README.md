## PPD

### Présentation

Le projet consiste à réaliser une application web pour mettre en place un outil permettant d’évaluer la qualité de données. À travers cette application, nous souhaitons évaluer la véracité des données en tenant en compte des différentes dimensions de la qualité de données

L’application a en entrée un jeu de données liés à des utilisateurs ayant pris le taxi  “Yellow Taxi Trip”.

L'application est développée en python avec la technologie streamlit, un outil opensource de création d'application web interactive pour le machine learning et la data science. Des fichiers de jeu de données sont récupérés par scrapping web ou par glisser-déposer. Une base de données MySQL charge les données. L'utilisateur peut ensuite sélectionner les attributs qu'il souhiate analyser via l'interface interactive de l'application.

### Installation

git clone https://github.com/Simras98/ppd-m2.git

Ouvrez dans un IDE : Pycharm

Vérifier que tous les packages sont bien installés. Ils sont répertoriés dans le requirement.txt

Ajouter une configuration d'éxécution :

![cd65aa2a7386f63864974f59fad67077.png](:/6c2004a1181b49668b6f6cd1306b9a01)

Lancer votre base de données MySQL en local avec uwamp par exemple.

Assurez vous dans le code que les identifiants de la base de données sont les bons.

Vous pouvez lancer l'application, rendez-vous sur votre navigateur.

### Utilisation

Pour tester l'application, vous pouvez utiliser à travers le module glisser-déposer une version réduite d'un des jeux de données concernés, il s'agit du fichier ????? à la racine du projet.

![8348debffb943d64a2d0cde8604235c5.png](:/0e954c72e5f443b49bb909395afcfaa7)

Une fois le fichier déposé, cliquez sur Suivant.
Le jeu de données est alors téléchargé et la base chargée avec.

Une fois les données chargées, vous pouvez choisir les attributs à analyser ou réiniatiliser la base.

![0b867264c3edbb25ce52518ea89fae66.png](:/aed9cd471cc840fdb24c68781369a809)

Cliquez sur Analyser

Une analyse globale sera afficher puis l'analyse spéficique en fonction des attributs précédements sélectionnés

![7ef333a6511ec3bf52a9a3babc400c29.png](:/e4bb633538cb463080e968fd911e9839)

Un tableau de présentation des contraintes vous sera afficher en bas de page.

![65bee87f5bd5a8624a5c830126873758.png](:/c2df40bfdcaf4816a59034c535ede0e3)








