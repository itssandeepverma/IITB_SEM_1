# CS699-Repo


Compilation or Running Instructions
 To start the server : python3 manage.py runserver in the main directory (CS699-Repo)
 Open on browser the local server i.e. 127.0.0.1:8000
 Fill the details asked in the dropdown boxes, click Submit button.


 Directory Structure :
  – manage.py : It helps to start the server via command line
  – project
    * urls.py : list all routes to views.py and called functions.  
    * Other files such as init .py, asgi.py, etc are present by default
      in project folder when we make a Django project.
  – scraping
     * templates/scraping
       · webpage.html : This is the main front-end webpage of the
            project. It includes the code for entering the data in HTML
            form, displaying the final information as well as plotting
            the bar graph via pyplot after getting required data from views.py.
            
     * urls.py list all the routes to view.py and called functions.
     * views.py : This .py file contains the main back-end portion of
      the code that is involved in scarping the data (using Selenium
      library) written in python. This return the dictionary request
      to webpage.html to display the information obtained in the front end.
      
    * Other files in this directory came by default when we create django web app.

 Libraries :
 – Selenium : Python open source library that helps with data scraping on a website using web elements.
 – matplotlib.pyplot : Used to plot bar graph of prices.
 – pyscript : A python library that enables us to write a python code within a HTML file by writing it within py-script tag.

