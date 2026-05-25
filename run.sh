#Docker Pull
docker pull mquintero27/vm_test:v1.0

#Docker Run
docker run --rm -v "$(pwd)/output:/tech_test_vm/output" docker.io/mquintero27/vm_test:v1.0