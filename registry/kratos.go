package registry

import "net/url"

func ParseEndpoint(endpoint string, scheme string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	if u.Scheme == scheme {
		return u.Host, nil
	}
	return "", nil
}
