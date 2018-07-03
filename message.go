package main

import (
	"encoding/json"
	"time"

	. "github.com/JiscRDSS/rdss-archivematica-channel-adapter/broker/message"
)

func encodeMessage(msg *Message) ([]byte, error) {
	return json.MarshalIndent(msg, "", "  ")
}

func createMessage() *Message {
	return &Message{
		MessageHeader: MessageHeader{
			ID:            NewUUID(),
			MessageClass:  MessageClassCommand,
			MessageType:   MessageTypeMetadataCreate,
			ReturnAddress: "string",
			MessageTimings: MessageTimings{
				PublishedTimestamp:  Timestamp(time.Date(2004, time.August, 1, 10, 0, 0, 0, time.UTC)),
				ExpirationTimestamp: Timestamp(time.Date(2004, time.August, 1, 10, 0, 0, 0, time.UTC)),
			},
			MessageSequence: MessageSequence{
				Sequence: NewUUID(),
				Position: 1,
				Total:    1,
			},
			MessageHistory: []MessageHistory{
				MessageHistory{
					MachineId:      "string",
					MachineAddress: "machine.example.com",
					Timestamp:      Timestamp(time.Date(2004, time.August, 1, 10, 0, 0, 0, time.UTC)),
				},
			},
			Version:   Version,
			Generator: "rdss-archivematica-msgcreator",
		},
		MessageBody: &MetadataCreateRequest{
			ResearchObject{
				ObjectUuid:  MustUUID("5680e8e0-28a5-4b20-948e-fd0d08781e0b"),
				ObjectTitle: "string",
				ObjectPersonRole: []PersonRole{
					PersonRole{
						Person: Person{
							PersonUuid: MustUUID("27811a4c-9cb5-4e6d-a069-5c19288fae58"),
							PersonIdentifier: []PersonIdentifier{
								PersonIdentifier{
									PersonIdentifierValue: "string",
									PersonIdentifierType:  PersonIdentifierTypeEnum_ORCID,
								},
							},
							PersonHonorificPrefix: "string",
							PersonGivenNames:      "string",
							PersonFamilyNames:     "string",
							PersonHonorificSuffix: "string",
							PersonMail:            "person@net",
							PersonOrganisationUnit: OrganisationUnit{
								OrganisationUnitUuid: MustUUID("28be7f16-0e70-461f-a2db-d9d7c64a8f17"),
								OrganisationUuidName: "string",
								Organisation: Organisation{
									OrganisationJiscId:  1,
									OrganisationName:    "string",
									OrganisationType:    OrganisationTypeEnum_charity,
									OrganisationAddress: "string",
								},
							},
						},
						Role: PersonRoleEnum_administrator,
					},
				},
				ObjectDescription: "string",
				ObjectRights: Rights{
					RightsStatement: []string{"string"},
					RightsHolder:    []string{"string"},
					Licence: []Licence{
						Licence{
							LicenceName:       "string",
							LicenceIdentifier: "string",
							LicenseStartDate:  Timestamp(time.Date(2018, time.January, 1, 0, 0, 0, 0, time.UTC)),
							LicenseEndDate:    Timestamp(time.Date(2018, time.December, 31, 23, 59, 59, 0, time.UTC)),
						},
					},
					Access: []Access{
						Access{
							AccessType:      AccessTypeEnum_open,
							AccessStatement: "string",
						},
					},
				},
				ObjectDate: []Date{
					Date{
						DateValue: "2002-10-02T10:00:00-05:00",
						DateType:  DateTypeEnum_accepted,
					},
				},
				ObjectKeywords:     []string{"string"},
				ObjectCategory:     []string{"string"},
				ObjectResourceType: ResourceTypeEnum_artDesignItem,
				ObjectValue:        ObjectValueEnum_normal,
				ObjectIdentifier: []Identifier{
					Identifier{
						IdentifierValue: "string",
						IdentifierType:  1,
					},
				},
				ObjectRelatedIdentifier: []IdentifierRelationship{
					IdentifierRelationship{
						Identifier: Identifier{
							IdentifierValue: "string",
							IdentifierType:  IdentifierTypeEnum_ARK,
						},
						RelationType: RelationTypeEnum_cites,
					},
				},
				ObjectOrganisationRole: []OrganisationRole{
					OrganisationRole{
						Organisation: Organisation{
							OrganisationJiscId:  1,
							OrganisationName:    "string",
							OrganisationType:    OrganisationTypeEnum_charity,
							OrganisationAddress: "string",
						},
						Role: OrganisationRoleEnum_funder,
					},
				},
				ObjectPreservationEvent: []PreservationEvent{
					PreservationEvent{
						PreservationEventValue:  "string",
						PreservationEventType:   PreservationEventTypeEnum_capture,
						PreservationEventDetail: "string",
					},
				},
				ObjectFile: []File{},
			},
		},
	}
}

func createFile(uuid, path, title, checksum string) *File {
	return &File{
		FileUUID:       MustUUID(uuid),
		FileIdentifier: uuid,
		FileName:       title,
		FileSize:       1,
		FileChecksum: []Checksum{
			Checksum{
				ChecksumUuid:  NewUUID(),
				ChecksumType:  ChecksumTypeEnum_md5,
				ChecksumValue: checksum,
			},
		},
		FileCompositionLevel: "string",
		FileDateModified:     []Timestamp{Timestamp(time.Date(2002, time.October, 2, 10, 0, 0, 0, time.FixedZone("", -18000)))},
		FileUse:              FileUseEnum_originalFile,
		FilePreservationEvent: []PreservationEvent{
			PreservationEvent{
				PreservationEventValue:  "string",
				PreservationEventType:   PreservationEventTypeEnum_capture,
				PreservationEventDetail: "string",
			},
		},
		FileUploadStatus:    UploadStatusEnum_uploadStarted,
		FileStorageStatus:   StorageStatusEnum_online,
		FileStorageLocation: path,
		FileStoragePlatform: FileStoragePlatform{
			StoragePlatformUuid: MustUUID("f2939501-2b2d-4e5c-9197-0daa57ccb621"),
			StoragePlatformName: "string",
			StoragePlatformType: StorageTypeEnum_S3,
			StoragePlatformCost: "string",
		},
	}
}
