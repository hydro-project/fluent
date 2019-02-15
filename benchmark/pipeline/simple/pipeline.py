# from: https://gist.github.com/simon-mo/5c9b951e0ead5463ff26d2a7300eba99

def preprocess(inp, batch_size=1):
    from sklearn import filters
    return filters.gaussian(inp).reshape(batch_size, 3, 224, 224)

def squeezenet(inp, batch_size):
    import torch, torchvision
    import numpy as np
    model = torchvision.models.squeezenet1_1()
    return model(torch.tensor(inp.astype(np.float32))).detach().numpy()

def average(inps):
    import numpy as np
    return np.mean(inps, axis=0)

def pipeline(inp, k_replicas=5):
    # stage one
    preprocess_output = preprocess(inp)

    # stage two
    squeezenets_output = [squeezenet(preprocess_output) for _ in range(k_replica)]

    # stage three
    final_result = average(squeezenets_output)

    return final_result

input_image = np.random.randn(batch_size, 224, 224, 3)
pipeline(input_image)
