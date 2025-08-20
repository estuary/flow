
# GCP BYOC Setup

If you want to use your own Google Cloud project for an Estuary Flow private deployment, you will first need to speak with your Estuary account manager. Estuary BYOC deployments require a license and additional setup on Estuary's side.

Once your account manager lets you know that the BYOC deployment can proceed, you will need to grant the `data-plane-controller@helpful-kingdom-273219.iam.gserviceaccount.com` principal Editor access to your project and give your Estuary point of contact your project id.

You also need to make sure your Google Cloud Project has Compute Engine API enabled, which you can enable by going to [APIs & Services -> API Library -> Compute Engine API](https://console.cloud.google.com/apis/library/compute.googleapis.com).
