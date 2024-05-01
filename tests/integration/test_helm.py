import subprocess
import time

def test_helm_chart_deployment():
    # Set the Helm chart name and release name
    chart_name = "cdf-fabric-replicator-chart"
    release_name = "int-test-helm"

    subprocess.run("pwd", shell=True, check=True)

    # Install the Helm chart
    install_command = f"helm install {release_name} {chart_name}"
    subprocess.run(install_command, shell=True, check=True)

    # Check if the release is deployed successfully
    status_command = f"helm status {release_name}"
    status_output = subprocess.check_output(status_command, shell=True).decode("utf-8")
    assert "STATUS: deployed" in status_output, "Helm chart deployment failed"

    # Add a 30-second sleep to allow time for the pods to start
    time.sleep(30)

    kube_status_command = f"kubectl get pods"
    kube_status_output = subprocess.check_output(kube_status_command, shell=True).decode("utf-8")
    print(kube_status_output)
    assert "Running" in kube_status_output, "Helm chart deployment failed to run in Kubernetes cluster"

    # Uninstall the Helm chart
    uninstall_command = f"helm uninstall {release_name}"
    subprocess.run(uninstall_command, shell=True, check=True)
