package test

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ExpectedStudy struct {
	ID primitive.ObjectID	`bson:"_id,omitempty"`
	RegistDate string		`bson:"rD,omitempty"`
	StudyDate string		`bson:"sD,omitempty"`
	HospitalName string		`bson:"h,omitempty"`
	RequestYN string		`bson:"rY,omitempty"`
	BackupYN string			`bson:"bY,omitempty"`
	StudyUID string			`bson:"stU,omitempty"`
	SeriesUID string		`bson:"seC,omitempty"`
	AddtionalBackupF string	`bson:"abf,omitempty"`
	MRequestY string 		`bson:"mrY,omitempty"`
	UploadInfo []UploadInfoData 	`bson:"uploadInfo,omitempty"`
}

type UploadInfoData struct {
	S3Key string			`bson:"s3key,omitempty"`
	Size int				`bson:"size,omitempty"`
	BackupDate string		`bson:"bD,omitempty"`
	SOPCount int			`bson:"soC,omitempty"`
}

var clientOptions *options.ClientOptions
var client *mongo.Client
var ctx context.Context

func GetHList() *[]string {
	bytes, err := ioutil.ReadFile("HList.dat")

	if err != nil {
		panic(err)
	}

	HList := strings.Split(string(bytes), "\n")      

	return &HList     
}

func TestFindDataToBeMod(t *testing.T){
	//Find mongoDB data
	mongoDBConnect()
    collection := client.Database("Dev").Collection("ExpectedStudy")

	//1. Get H name
	HList := GetHList()

	for _, hosName := range *HList {

		hosName := strings.TrimSpace(hosName)

		needModDatafile, err := os.Create("modifiedUploadInfoFile_"+ hosName +".dat")
		defer needModDatafile.Close()
	
		if err != nil {
			fmt.Println(err)
			return
		}

		//findResultCursor, err := collection.Find(ctx, bson.D{})
		findResultCursor, err := collection.Find(ctx, bson.M{"h":hosName, "sD":bson.M{"$gte":"20210301","$lte":"20210331"}, "uploadInfo":bson.M{"$size":2}})

		if err != nil {
			fmt.Println("MongoDB Find Error : ", err)
		}

		if err = findResultCursor.All(ctx, bson.D{}); err != nil {
			fmt.Println(err)
		}

		var listCount int
		for findResultCursor.Next(ctx) {
			var findResult ExpectedStudy
			err := findResultCursor.Decode(&findResult)

			if err != nil {
				fmt.Println("cursor.Next() error :",err)
				os.Exit(1)
			} else {
				fmt.Println("result type :", reflect.TypeOf(findResult))
				fmt.Println("result :", findResult)

				haveDCMFile,  needChangeUploadInfoData := false, false

				var prvUploadInfo UploadInfoData
				var newUploadInfo UploadInfoData

				newExpectedStudy := findResult
				newExpectedStudy.UploadInfo = nil

				for idx, uploadInfoValue := range findResult.UploadInfo {
					if strings.Contains(uploadInfoValue.S3Key,".dcm") {
						haveDCMFile = true
					}

					if idx == 0 {
						prvUploadInfo = uploadInfoValue
					} else if !haveDCMFile && idx == 1 {
						if uploadInfoValue.S3Key == prvUploadInfo.S3Key && 
						uploadInfoValue.Size == prvUploadInfo.Size &&
						uploadInfoValue.BackupDate == prvUploadInfo.BackupDate {

							newUploadInfo.S3Key = uploadInfoValue.S3Key
							newUploadInfo.Size = uploadInfoValue.Size
							newUploadInfo.BackupDate = uploadInfoValue.BackupDate
							newUploadInfo.SOPCount = uploadInfoValue.SOPCount + prvUploadInfo.SOPCount
							
							newExpectedStudy.UploadInfo = append(newExpectedStudy.UploadInfo, newUploadInfo)

							needChangeUploadInfoData = true
						}
					}
				}

				if !haveDCMFile && needChangeUploadInfoData {
					listCount ++
					//업데이트 필요 ID를 파일로 저장
					fmt.Fprintln(needModDatafile, findResult.ID.Hex())
					//ioutil.WriteFile("modifiedUploadInfoFile_"+ strings.TrimSpace(hosName) +".dat", []byte(findResult.ID.Hex()), 0777)
				}
			}
		}

		if listCount == 0 {
			err := os.Remove("modifiedUploadInfoFile_"+ hosName +".dat")
			
			if err != nil {
				panic(err)
			}
		}
	}

	client.Disconnect(ctx)
}

func mongoDBConnect(){
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)

    clientOptions = options.Client().ApplyURI("mongodb://localhost:27017")

	var err error
    client, err = mongo.Connect(ctx, clientOptions)

    if err != nil { 
		fmt.Println("MongoDB Connection Error : ", err)
    }

    err = client.Ping(ctx, nil)

    if err != nil {
        fmt.Println("MongoDB Ping Error : ", err)
    }
}