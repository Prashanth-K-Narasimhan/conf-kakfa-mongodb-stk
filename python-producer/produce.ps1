Get-Content /mnt/data/use_case1.json | docker run --rm -i --network host confluentinc/cp-kafka:7.4.1 `
  kafka-console-producer --bootstrap-server localhost:29092 --topic telemetry
