package api

import (
	"context"

	admissiondaemon "github.com/xiaoheiCat/OpenTuttiVM/packages/desktop/update-admission/daemon"
	tuttigenerated "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/api/generated"
	"github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/apierrors"
)

func (api DaemonAPI) GetDesktopUpdateAdmissionSnapshot(
	_ context.Context,
	_ tuttigenerated.GetDesktopUpdateAdmissionSnapshotRequestObject,
) (tuttigenerated.GetDesktopUpdateAdmissionSnapshotResponseObject, error) {
	if api.DesktopUpdateAdmissionService == nil {
		return tuttigenerated.GetDesktopUpdateAdmissionSnapshot503JSONResponse{
			ServiceUnavailableErrorJSONResponse: desktopUpdateAdmissionUnavailableError(),
		}, nil
	}
	return tuttigenerated.GetDesktopUpdateAdmissionSnapshot200JSONResponse(
		generatedDesktopUpdateAdmissionSnapshot(api.DesktopUpdateAdmissionService.Snapshot()),
	), nil
}

func (api DaemonAPI) GetDesktopUpdateAdmissionStartup(
	ctx context.Context,
	_ tuttigenerated.GetDesktopUpdateAdmissionStartupRequestObject,
) (tuttigenerated.GetDesktopUpdateAdmissionStartupResponseObject, error) {
	if api.DesktopUpdateAdmissionService == nil {
		return tuttigenerated.GetDesktopUpdateAdmissionStartup503JSONResponse{
			ServiceUnavailableErrorJSONResponse: desktopUpdateAdmissionUnavailableError(),
		}, nil
	}
	snapshot, err := api.DesktopUpdateAdmissionService.WaitInitial(ctx)
	if err != nil {
		return tuttigenerated.GetDesktopUpdateAdmissionStartup503JSONResponse{
			ServiceUnavailableErrorJSONResponse: serviceUnavailableError(
				apierrors.ServiceUnavailable(
					"desktop_update_admission_startup_unavailable",
					apierrors.WithCause(err),
				),
			),
		}, nil
	}
	return tuttigenerated.GetDesktopUpdateAdmissionStartup200JSONResponse(
		generatedDesktopUpdateAdmissionSnapshot(snapshot),
	), nil
}

func (api DaemonAPI) RefreshDesktopUpdateAdmission(
	ctx context.Context,
	request tuttigenerated.RefreshDesktopUpdateAdmissionRequestObject,
) (tuttigenerated.RefreshDesktopUpdateAdmissionResponseObject, error) {
	if request.Body == nil {
		return tuttigenerated.RefreshDesktopUpdateAdmission400JSONResponse{
			InvalidRequestErrorJSONResponse: invalidRequestError(
				apierrors.EmptyBody(apierrors.WithDeveloperMessage("empty body")),
			),
		}, nil
	}
	if !request.Body.Trigger.Valid() {
		return tuttigenerated.RefreshDesktopUpdateAdmission400JSONResponse{
			InvalidRequestErrorJSONResponse: invalidRequestError(
				apierrors.MalformedRequest(
					apierrors.WithDeveloperMessage("trigger must be foreground or retry"),
				),
			),
		}, nil
	}
	if api.DesktopUpdateAdmissionService == nil {
		return tuttigenerated.RefreshDesktopUpdateAdmission503JSONResponse{
			ServiceUnavailableErrorJSONResponse: desktopUpdateAdmissionUnavailableError(),
		}, nil
	}
	result, err := api.DesktopUpdateAdmissionService.Refresh(
		ctx,
		admissiondaemon.RefreshTrigger(request.Body.Trigger),
	)
	if err != nil {
		return tuttigenerated.RefreshDesktopUpdateAdmission503JSONResponse{
			ServiceUnavailableErrorJSONResponse: serviceUnavailableError(
				apierrors.ServiceUnavailable(
					"desktop_update_admission_refresh_unavailable",
					apierrors.WithCause(err),
				),
			),
		}, nil
	}
	return tuttigenerated.RefreshDesktopUpdateAdmission200JSONResponse(
		generatedDesktopUpdateAdmissionRefreshResult(result),
	), nil
}

func desktopUpdateAdmissionUnavailableError() tuttigenerated.ServiceUnavailableErrorJSONResponse {
	return serviceUnavailableError(
		apierrors.ServiceUnavailable("desktop_update_admission_service_unavailable"),
	)
}

func generatedDesktopUpdateAdmissionRefreshResult(
	result admissiondaemon.RefreshResult,
) tuttigenerated.DesktopUpdateAdmissionRefreshResult {
	var skipReason *tuttigenerated.DesktopUpdateAdmissionRefreshResultSkipReason
	if result.SkipReason != "" {
		value := tuttigenerated.DesktopUpdateAdmissionRefreshResultSkipReason(result.SkipReason)
		skipReason = &value
	}
	return tuttigenerated.DesktopUpdateAdmissionRefreshResult{
		Performed:  result.Performed,
		SkipReason: skipReason,
		Snapshot:   generatedDesktopUpdateAdmissionSnapshot(result.Snapshot),
	}
}

func generatedDesktopUpdateAdmissionSnapshot(
	snapshot admissiondaemon.Snapshot,
) tuttigenerated.DesktopUpdateAdmissionSnapshot {
	return tuttigenerated.DesktopUpdateAdmissionSnapshot{
		Identity: tuttigenerated.DesktopUpdateAdmissionIdentity{
			Product:        tuttigenerated.DesktopUpdateAdmissionProduct(snapshot.Identity.Product),
			Platform:       tuttigenerated.DesktopUpdateAdmissionPlatform(snapshot.Identity.Platform),
			Architecture:   tuttigenerated.DesktopUpdateAdmissionArchitecture(snapshot.Identity.Architecture),
			CurrentVersion: snapshot.Identity.CurrentVersion,
		},
		Policy:                generatedDesktopUpdateAdmissionPolicy(snapshot.Policy),
		FeatureAvailability:   generatedDesktopUpdateAdmissionFeature(snapshot.FeatureAvailability),
		LastAttemptAt:         snapshot.LastAttemptAt,
		NextForegroundCheckAt: snapshot.NextForegroundCheckAt,
	}
}

func generatedDesktopUpdateAdmissionPolicy(
	policy admissiondaemon.PolicySnapshot,
) tuttigenerated.DesktopUpdateAdmissionPolicySnapshot {
	result := tuttigenerated.DesktopUpdateAdmissionPolicySnapshot{
		Status: tuttigenerated.DesktopUpdateAdmissionPolicySnapshotStatus(policy.Status),
	}
	if policy.Response != nil {
		response := tuttigenerated.DesktopUpdateAdmissionPolicyResponse{
			Channel:        tuttigenerated.DesktopUpdateAdmissionPolicyResponseChannel(policy.Response.Channel),
			Decision:       tuttigenerated.DesktopUpdateAdmissionPolicyResponseDecision(policy.Response.Decision),
			PolicyRevision: policy.Response.PolicyRevision,
			Reason:         tuttigenerated.DesktopUpdateAdmissionPolicyResponseReason(policy.Response.Reason),
		}
		if policy.Response.MinimumVersion != "" {
			response.MinimumVersion = &policy.Response.MinimumVersion
		}
		result.Response = &response
	}
	if policy.Failure != nil {
		result.Failure = &tuttigenerated.DesktopUpdateAdmissionPolicyFailure{
			Kind: tuttigenerated.DesktopUpdateAdmissionPolicyFailureKind(policy.Failure.Kind),
		}
	}
	if policy.Reason != "" {
		reason := tuttigenerated.DesktopUpdateAdmissionPolicySnapshotReason(policy.Reason)
		result.Reason = &reason
	}
	return result
}

func generatedDesktopUpdateAdmissionFeature(
	feature admissiondaemon.FeatureAvailabilitySnapshot,
) tuttigenerated.DesktopUpdateAdmissionFeatureAvailability {
	keys := append([]string(nil), feature.Keys...)
	if keys == nil {
		keys = []string{}
	}
	return tuttigenerated.DesktopUpdateAdmissionFeatureAvailability{
		Keys:           keys,
		Source:         tuttigenerated.DesktopUpdateAdmissionFeatureAvailabilitySource(feature.Source),
		PolicyRevision: feature.PolicyRevision,
		FetchedAt:      feature.FetchedAt,
	}
}
