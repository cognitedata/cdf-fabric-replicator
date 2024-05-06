import subprocess
from time import sleep

def test_helm_chart_deployment(retries=5, running_time=60):
    # Set the Helm chart name and release name
    chart_name = "cdf-fabric-replicator-chart"
    release_name = "int-test-helm"

    #Ensure the helm chart is not installed
    print("Uninstalling the Helm chart if it is already installed...")
    uninstall_command = f"helm uninstall {release_name}"
    subprocess.run(uninstall_command, shell=True)
    
    # Install the Helm chart
    install_command = f"helm install {release_name} {chart_name}"
    subprocess.run(install_command, shell=True, check=True)

    # Check if the release is deployed successfully
    status_command = f"helm status {release_name}"
    status_output = subprocess.check_output(status_command, shell=True).decode("utf-8")
    assert "STATUS: deployed" in status_output, "Helm chart deployment failed"
    
    kube_status_command = "kubectl get pods"
    for i in range(retries):
        sleep(2**i)  # wait before the next retry using exponential backoff
        
        kube_status_output = subprocess.check_output(kube_status_command, shell=True).decode("utf-8")
        print(kube_status_output)

        if "Running" in kube_status_output:
            break  # Pod is up, break the loop
        else:
            print(f"Waiting for pod to start, retrying... ({i+1}/{retries})")
    else:
        # If we've reached this point, the pod is not up after the maximum number of retries
        assert False, "Pod did not start up within the expected time"
        
    # Check that the pod stays running for the specified running time
    for _ in range(running_time):
        sleep(1)  # wait for 1 second

        kube_status_output = subprocess.check_output(kube_status_command, shell=True).decode("utf-8")
        print(kube_status_output)

        assert "Running" in kube_status_output, "Pod did not stay running"

    # Uninstall the Helm chart
    uninstall_command = f"helm uninstall {release_name}"
    subprocess.run(uninstall_command, shell=True, check=True)
