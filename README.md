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

![This is an image](https://github.com/Simras98/ppd-m2/blob/db457ed258a96c25dc5f15bf4b1ce2e1283ea5ba/cd65aa2a7386f63864974f59fad67077.png)

Lancer votre base de données MySQL en local avec uwamp par exemple.

Assurez vous dans le code que les identifiants de la base de données sont les bons.

Vous pouvez lancer l'application, rendez-vous sur votre navigateur.

### Utilisation

Pour tester l'application, vous pouvez utiliser à travers le module glisser-déposer une version réduite d'un des jeux de données concernés, il s'agit du fichier ????? à la racine du projet.

![This is an image]()

Une fois le fichier déposé, cliquez sur Suivant.
Le jeu de données est alors téléchargé et la base chargée avec.

Une fois les données chargées, vous pouvez choisir les attributs à analyser ou réiniatiliser la base.

![This is an image]()

Cliquez sur Analyser

Une analyse globale sera afficher puis l'analyse spéficique en fonction des attributs précédements sélectionnés

![This is an image]()

Un tableau de présentation des contraintes vous sera afficher en bas de page.

![This is an image]()








