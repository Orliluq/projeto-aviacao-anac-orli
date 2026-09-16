## ⚙️ Conclusão
Até aqui, consegui entender melhor como funciona o fluxo de dados no Databricks, desde a ingestão até a organização nas camadas Bronze e Silver.

Na **Bronze**, os dados do VRA foram ingeridos mantendo os registros próximos do formato original. No total, foram carregados **1.014.705 registros de 12 arquivos CSV**, além das colunas de auditoria.

Também criei e validei as tabelas de referência de aeroportos, empresas e códigos de operação.

Na **Silver**, pude aplicar tipagem, organização e governança sem alterar a quantidade de registros da tabela VRA. A validação mostrou **1.014.705 registros na Bronze e 1.014.705 na Silver**, então nenhuma linha foi perdida nessa etapa.

Uma coisa que ficou bem clara para mim foi a diferença entre as camadas: na Bronze, o foco é preservar os dados, enquanto na Silver é possível organizar, tipar e documentar os dados sem colocar regras de negócio.

Também tive alguns problemas de compatibilidade com o pipeline de qualidade e com a sintaxe mais antiga do material, mas isso também fez parte do aprendizado e me ajudou a entender melhor como o Databricks está trabalhando atualmente.

Até aqui, foi uma boa prática para entender na prática o caminho **Bronze → Silver** e a importância de validar cada etapa do pipeline. 🚀

---

--- 🇪🇸
## ⚙️ Conclusión
Hasta ahora pude entender mejor cómo funciona el flujo de datos en Databricks, desde la ingesta hasta la organización en las capas Bronze y Silver.

En **Bronze**, los datos del VRA fueron ingeridos manteniendo los registros lo más cerca posible del formato original. En total, se cargaron **1.014.705 registros de 12 archivos CSV**, además de las columnas de auditoría.

También creé y validé las tablas de referencia de aeropuertos, empresas y códigos de operación.

En **Silver**, pude aplicar tipificación, organización y gobernanza sin cambiar la cantidad de registros de la tabla VRA. La validación mostró **1.014.705 registros en Bronze y 1.014.705 en Silver**, por lo que no se perdió ninguna fila en esta etapa.

Algo que me quedó bastante claro es la diferencia entre las capas: en Bronze, el foco está en preservar los datos, mientras que en Silver se pueden organizar, tipificar y documentar sin introducir reglas de negocio.

También tuve algunos problemas de compatibilidad con el pipeline de calidad y con la sintaxis más antigua del material, pero eso también fue parte del aprendizaje y me ayudó a entender mejor cómo funciona actualmente Databricks.

Hasta aquí, fue una buena práctica para entender en la práctica el flujo **Bronze → Silver** y la importancia de validar cada etapa del pipeline. 🚀