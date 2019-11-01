#	Next generation data processing algorithms and pipeline components for (most) NEON TIS and AIS data products.

##  NOTE: The processing algorithms in this repository are in development and not currently used to produce data on the NEON portal. 

##  High-level organization

- /flow: contains workflow templates/executables for generic processing modules (e.g. calibration). Dockerfiles included.
- /pack: contains packages/libraries (for reusable function calls, like reading/writing data files). Dockerfiles included. 
- /pipe: contains Pachyderm pipeline specifications. Includes associated code/dockerfiles if applicable only to that pipeline. 


## Credits & Acknowledgements


<!-- HTML tags to produce image, resize, add hyperlink. -->
<!-- ONLY WORKS WITH HTML or GITHUB documents -->
<a href="http://www.neonscience.org/">
<img src="logo.png" width="300px" />
</a>

<!-- Acknowledgements text -->
The National Ecological Observatory Network is a project solely funded by the National Science Foundation and managed under cooperative agreement by Battelle. Any opinions, findings, and conclusions or recommendations expressed in this material are those of the author(s) and do not necessarily reflect the views of the National Science Foundation.


<!-- ****** License ****** -->
## License
GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007



<!-- ****** Disclaimer ****** -->
## Disclaimer
*Information and documents contained within this repository are available as-is. Codes or documents, or their use, may not be supported or maintained under any program or service and may not be compatible with data currently available from the NEON Data Portal.*
