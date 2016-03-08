package controller

func (c *Controller) cmdAuth(line string) error {
	var password string
	if line, password = token(line); password == "" {
		return errInvalidNumberOfArguments
	}
	if line != "" {
		return errInvalidNumberOfArguments
	}

	println(password)

	return nil
}
