import { Injectable } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';

@Injectable({
  providedIn: 'root'
})
export class QueryParameterService {
  constructor(
    private readonly activatedRoute: ActivatedRoute,
    private readonly router: Router
  ) {}

  public getIdsFromQueryParameters(): Promise<string[]> {
    return new Promise(resolve => {
      this.activatedRoute.queryParamMap.subscribe(params => {
        if (params.has('id')) {
          const ids = params.getAll('id');
          resolve(ids);
        }
      });
    });
  }

  public addQueryParameter(transformationId: string): void {
    let url = this.router.url;

    if (this.activatedRoute.snapshot.queryParamMap.has('id')) {
      const ids = this.activatedRoute.snapshot.queryParamMap.getAll('id');
      if (ids.find(id => id === transformationId) === undefined) {
        url += `&id=${transformationId}`;
      }
    } else {
      url += `?id=${transformationId}`;
    }

    this.router.navigateByUrl(url);
  }

  public deleteQueryParameter(transformationId: string): void {
    let url = this.router.url;

    if (this.activatedRoute.snapshot.queryParamMap.has('id')) {
      const ids = this.activatedRoute.snapshot.queryParamMap.getAll('id');

      if (ids.length > 1) {
        for (let i = 0; i < ids.length; i++) {
          if (ids[i] === transformationId) {
            if (i === ids.length - 1) {
              url = url.replace(`&id=${transformationId}`, '');
            } else {
              url = url.replace(`id=${transformationId}&`, '');
            }
          }
        }
      } else {
        url = url.replace(`/?id=${transformationId}`, '');
      }

      this.router.navigateByUrl(url);
    }
  }
}
