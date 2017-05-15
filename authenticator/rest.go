package authenticator

import (
	"log"

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

// Authenticate : todo
func Authenticate(url string, path string, headerName string, headerValue string, authenticationToken string) (user.ID, string, error) {
	notificationServerBase := sling.New().Base(url).Set(headerName, headerValue)

	body := &AuthenticationMessage{AuthenticationToken: authenticationToken}
	req, err := notificationServerBase.New().Post(path).BodyJSON(body).Request()
	if err != nil {
		log.Printf("Request error %v", err)
		return user.ID{}, "", err
	}

	successfulResponse := AuthenticationResponse{}
	_, responseErr := notificationServerBase.Do(req, &successfulResponse, &successfulResponse)
	if responseErr != nil {
		log.Printf("responseError:%v", responseErr)
		return user.ID{}, "", responseErr
	}

	userID, _ := user.NewID(successfulResponse.UserID)
	log.Printf("Received userId:%v and username:%v", userID, successfulResponse.Username)
	return userID, successfulResponse.Username, nil
}
