package authenticator

import (
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/dghubble/sling"
	"github.com/piot/hasty-protocol/user"
)

// AuthenticationMessage : todo
type AuthenticationMessage struct {
	AuthenticationToken string `json:"at"`
}

// AuthenticationResponse : todo
type AuthenticationResponse struct {
	SessionID          string `json:"sessionId"`
	UserID             uint64 `json:"userId"`
	LastLoginTimestamp uint64 `json:"lastLoginTimestamp"`
	Username           string `json:"username"`
}

// ErrorResponse : todo
type AuthenticationErrorResponse struct {
	ErrorCode    int    `json:"errorCode"`
	ErrorMessage string `json:"errorMessage"`
}

// Authenticate : todo
func Authenticate(url string, path string, headerName string, headerValue string, authenticationToken string) (user.ID, string, error) {
	notificationServerBase := sling.New().Base(url).Set(headerName, headerValue)

	body := &AuthenticationMessage{AuthenticationToken: authenticationToken}
	req, err := notificationServerBase.New().Post(path).BodyJSON(body).Request()
	if err != nil {
		log.Warnf("Request error %v", err)
		return user.ID{}, "", err
	}

	successfulResponse := AuthenticationResponse{}
	unsuccessfulResponse := AuthenticationErrorResponse{}
	response, responseErr := notificationServerBase.Do(req, &successfulResponse, &unsuccessfulResponse)
	if responseErr != nil {
		log.Warnf("responseError:%v", responseErr)
		return user.ID{}, "", responseErr
	}

	if response.StatusCode < 200 || response.StatusCode > 299 {
		authenticationError := fmt.Errorf("Could not authenticate: HTTP error %d. %d - '%s'", response.StatusCode, unsuccessfulResponse.ErrorCode, unsuccessfulResponse.ErrorMessage)
		log.Warnf("authenticationError:%s", authenticationError)
		return user.ID{}, "", authenticationError
	}

	userID, _ := user.NewID(successfulResponse.UserID)
	log.Infof("Received userId:%v and username:%v", userID, successfulResponse.Username)
	return userID, successfulResponse.Username, nil
}
