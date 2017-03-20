
## weather
```
graph LR
      A[Job] -->B(MAP)
      B -->C{分区}
      C -->|第一个分区| D[sort] 
      C -->|第二个分区| E[sort] 
      C -->|第三个分区| F[sort] 
      D -->G[分组]
      E -->W[分组]
      F -->Z[分组]
```