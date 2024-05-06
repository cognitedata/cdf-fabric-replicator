import subprocess
from time import sleep

def uninstall_helm_chart(release_name):
    uninstall_command = f"helm uninstall {release_name}"
    subprocess.run(uninstall_command, shell=True)
    
def install_helm_chart(release_name, chart_name):
    install_command = f"helm install {release_name} {chart_name}"
    subprocess.run(install_command, shell=True, check=True)
    
def check_pod_status():
    kube_status_command = "kubectl get pods"
    kube_status_output = subprocess.check_output(kube_status_command, shell=True).decode("utf-8")
    print(kube_status_output)
    return kube_status_output

def wait_for_pod_to_start(retries):
    for i in range(retries):
        sleep(2**i)  # wait before the next retry using exponential backoff
        
        kube_status_output = check_pod_status()

        if "Running" in kube_status_output:
            break  # Pod is up, break the loop
        else:
            print(f"Waiting for pod to start, retrying... ({i+1}/{retries})")
    else:
        # If we've reached this point, the pod is not up after the maximum number of retries
        assert False, "Pod did not start up within the expected time"
        
def assert_pod_stays_running(running_time):
    for _ in range(running_time):
        sleep(1)  # wait for 1 second

        kube_status_output = check_pod_status()

        assert "Running" in kube_status_output, "Pod did not stay running"
        
def assert_helm_deployed(release_name):
    # Check if the release is deployed successfully
    status_command = f"helm status {release_name}"
    status_output = subprocess.check_output(status_command, shell=True).decode("utf-8")
    assert "STATUS: deployed" in status_output, "Helm chart deployment failed"

def test_helm_chart_deployment(retries=5, running_time=60):
    # Set the Helm chart name and release name
    chart_name = "cdf-fabric-replicator-chart"
    release_name = "int-test-helm"

    #Ensure the helm chart is not installed
    print("Uninstalling the Helm chart if it is already installed...")
    uninstall_helm_chart(release_name)
    
    # Install the Helm chart
    install_helm_chart(release_name, chart_name)

    assert_helm_deployed(release_name)
    
    wait_for_pod_to_start(retries)
        
    # Check that the pod stays running for the specified running time
    assert_pod_stays_running(running_time)

    # Uninstall the Helm chart
    uninstall_helm_chart(release_name)
